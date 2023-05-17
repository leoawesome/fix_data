const arrayOfObjectsDifference = (s3Payload, esPayload, fields = []) => {
    return s3Payload.filter(s3Item => {
        const esItems = esPayload.find((esItem) => {
            return fields.every((field) => {
                return s3Item[field] === esItem[field];
            });
        });
        return !esItems;
    });
};

console.log(arrayOfObjectsDifference(
    [
        {
            id: 1,
            name: 'test1',
            role: 'admin',
        },
        {
            id: 2,
            name: 'test2',
        }],
    [
        {
            id: 1,
            name: 'test1',
        },
        {
            id: 2,
            name: 'test2',
        }],
    ['id', 'name'],
));
