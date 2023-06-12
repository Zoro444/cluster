const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const cluster = require('cluster');

const csvDirectory = process.argv[2];

if (!csvDirectory) {
  console.error('Select csv directory path!');
  process.exit();
}

if (cluster.isPrimary) {
  const numCPUs = require('os').cpus().length;
  let csvFiles = [];
  let parsedCsv = [];

  fs.promises.readdir(csvDirectory)
    .then((data) => {      
       csvFiles = data.filter((csvFile) => path.extname(csvFile) === '.csv');
       for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
       } 
       fs.promises.mkdir(path.join(csvDirectory, 'converted'))
        .then((data) => {

        })
        .catch((err) => {
            
        });   
    })
    .catch((err) => {
        console.log('No such directory!');
    });

  let fileIndex = -1;

  cluster.on('online', worker => {
    if (csvFiles.length - 1 > fileIndex) {
        fileIndex++;
        worker.send({ filePath: path.join(csvDirectory, csvFiles[fileIndex]), fileName: csvFiles[fileIndex] });     
    }
    else {
        worker.send({ end: 'end' });
    }

    worker.on('message', message => {
        parsedCsv.push(message)
      });
  });

  cluster.on('exit', (data) => {
    if (Object.keys(cluster.workers).length === 0) {
        console.log(parsedCsv);
    }
    if (csvFiles.length - 1 > fileIndex) {
        cluster.fork();
    }
  });

} 
else {
  process.on('message', message => {  
    if(message.end) {
      process.exit();
    }

    let start;


    const csvDataWriteJson = fs.createWriteStream(
        path.join(
            path.resolve(), csvDirectory, 'converted', 
            `${path.basename(message.fileName, path.extname(message.fileName))}.json`
        ), "utf-8"
    );
    csvDataWriteJson.write('[');
    let firstData = true;

    const csvData = fs.createReadStream(path.join(path.resolve(), message.filePath), "utf-8")
        .on('open', () => { start = new Date();})
        .pipe(csv())
        .on('data', (data) => {
            csvDataWriteJson.write(firstData 
                ? JSON.stringify(data)
                : `,${JSON.stringify(data)}`
            );
            firstData = false;
        })
        .on('end', () => {
            csvDataWriteJson.write(']');

            csvDataWriteJson.end((err) => {
                if (err) {
                  console.error('Error writing to file:', err);
                  return;
                }
                console.log('Data written to output.json file');

              });
        });

    csvData.on('close', (data) => {
      let parsingInfo = {
        fileName: message.filePath,
        duration: (new Date() - start) + "ms",
      }   
      process.send(parsingInfo);

      setTimeout(process.exit);
    })    
  });
}
