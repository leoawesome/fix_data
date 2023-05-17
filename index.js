const fs = require('fs').promises;

const { Client } = require('@opensearch-project/opensearch');

async function readFile(fileName) {
  const fileBuffer = await fs.readFile(`files/${fileName}`, 'utf8');
  const fileString = fileBuffer.toString();
  const fileArray = fileString.split('\n');
  const data = fileArray.map((v) => {
    try {
      if (v === '') {
        return null;
      }
      return JSON.parse(v);
    } catch (e) {
      console.log(e);
      return null;
    }
  }).filter(Boolean);
  return data;
}

async function readFiles(filePaths) {
  const dataArray = await Promise.all(filePaths.map(async (v) => {
    const fileData = await readFile(v);
    console.log('length of the file', fileData.length);
    return fileData;
  }));
  return dataArray.flat(1);
}

async function writeToFile(fileName, data) {
  const lines = data.map((obj) => JSON.stringify(obj));
  const content = lines.join('\n');
  await fs.writeFile(`files/${fileName}`, content);
}
const arrayOfObjectsDifference = (s3Payload, esPayload, fields = []) => s3Payload.filter((s3Item) => {
  const esItems = esPayload.find((esItem) => fields.every((field) => s3Item[field] === esItem[field]));
  return !esItems;
});

async function ingestDataInEs(body, esClient, indexName) {
  const batchSize = 10000;
  const total = body.length;
  const batches = Math.ceil(total / batchSize);
  for (let i = 0; i < batches; i++) {
    const start = i * batchSize;
    const end = start + batchSize;
    const { body: bulkResponse } = await esClient.bulk({
        refresh: true,
        body: body.slice(start, end).flatMap((doc) => [
            { index: { _index: indexName } },
            doc,
        ]),
    });
    if (bulkResponse.errors) {
      const erroredDocuments = [];
      bulkResponse.items.forEach((action, i) => {
        const operation = Object.keys(action)[0];
        if (action[operation].error) {
          erroredDocuments.push({
            status: action[operation].status,
            error: action[operation].error,
            operation: body[i * 2],
            document: body[i * 2 + 1],
          });
        }
      });
      console.log(erroredDocuments);
    }
  }
}

(async () => {
  try {
    const filePaths = ['1'];
    const data = await readFiles(filePaths);
    const esData = [];
    const filteredData = arrayOfObjectsDifference(data, esData, ['id', 'name']);
    console.log('filtered data length', filteredData.length);
    await writeToFile('filteredData.csv', filteredData);
    console.log('total length', data.length);
    console.log(data[data.length - 1]);
    console.log(Math.max(...data.map((v) => v.createdAt)));
    const filePathEs = ['filteredData.csv'];
    const dataEs = await readFiles(filePathEs);
    const esClient = new Client({
      node: 'https://admin:admin@localhost:9201'
    });
    await ingestDataInEs(dataEs, esClient, 'message-000002');
    console.log('total length', dataEs.length);
  } catch (err) {
    console.log(err);
  }
})();
