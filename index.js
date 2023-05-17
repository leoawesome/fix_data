const fs = require('fs').promises;

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

(async () => {
  try {
    const filePaths = ['1', '2', '3', '4'];
    const data = await readFiles(filePaths);
    console.log('total length', data.length);
    console.log(data[data.length - 1]);
    console.log(Math.max(...data.map((v) => v.createdAt)));
  } catch (err) {
    console.log(err);
  }
})();
