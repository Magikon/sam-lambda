const _ = require('lodash');
const Excel = require('exceljs');
const XLSX = require('xlsx');

function getAccountBy(key, value, accounts) {
    // Find the account object the value of a property matches a given value

    const account = accounts.find(a => {
        return a[key] === value
    });

    return account;
}

function getFileData(workbook, columns, accounts, isGlobal = false) {
    const combinations = [];

    //const parsedAccounts = []
    // Iterate through each sheet
    workbook.eachSheet((worksheet) => {

        let account;
        // Get account id for this sheet name is case not global
        if (!isGlobal) account = getAccountBy('name', String(worksheet.name).toLowerCase(), accounts);

        // Iterate through all rows and pull specific columns
        // And return a list with appended account id's
        if (account || isGlobal) {

            //Log parsed accounts
            //parsedAccounts.push(account.id)

            const lastRow = worksheet.lastRow.number;
            for (let i = 2; i <= lastRow; i++) {
                const row = worksheet.getRow(i);
                let record = {};
                if (!isGlobal) record = { 'account_id': account.id };
                _.map(columns, c => {
                    let value = row.getCell(c.coordinate).value;
                    if(String(value).trim() == "") value = null;
                    record[c.name] = value;
                });
                combinations.push(record);
            }
        }
        
    });
    
    // Check for new accounts.
    // If found.. add a row for given account
    //accounts.forEach( a => {
    //    if (!parsedAccounts.includes(a.id)) {
    //        console.log(`::::: Found new Account! Appending.. Name: ${a.name}, ID: ${a.id}`)
    //        let newRecord = { 'account_id': a.id };
    //        _.map(columns, c => {
    //            newRecord[c.name] = "1";
    //        });
    //        combinations.push(newRecord);
    //    }
    //})

    console.log("::::: Successfully Read File")
    return combinations;
}

function appendFile(workbook, data) {
    _.mapKeys(workbook.Sheets, (worksheet, sheetName) => {
        const worksheetData = data[sheetName.toLowerCase()];
        if (worksheetData) {
            for (let row of worksheetData) {
                XLSX.utils.sheet_add_aoa(worksheet, [row], { origin: -1 });
            }
        }
    });
}

function formatData(data, accounts, columns) {
    const dataBySheets = {};
    _.forEach(data, rowData => {
        const account = getAccountBy('id', rowData['account_id'], accounts);
        if (account) {
            const sheetName = account.name.toLowerCase();
            if (!dataBySheets[sheetName]) {
                dataBySheets[sheetName] = [];
            }
            const row = [];
            _.forEach(columns, column => {
                row.push(rowData[column.name]);
            });
            dataBySheets[sheetName].push(row);
        }
    });
    return dataBySheets;
}
function createWorksheet(workbook, columns, name) {
    console.log(`::::: Creating Worksheet for Account: ${name}`)

    var keys = columns.map(c => c.name)
    var ws = XLSX.utils.aoa_to_sheet([keys])

    XLSX.utils.book_append_sheet(workbook, ws, name)
}
function createWorkbookFile (workbook, data, keys) {
    for (let sheetName in data) {
        const worksheetData = data[sheetName.toLowerCase()];
        let ws = XLSX.utils.aoa_to_sheet([keys])
        if (worksheetData) {
            for (let row of worksheetData) {
                XLSX.utils.sheet_add_aoa(ws, [row], { origin: -1 });
            }
            workbook.Sheets[sheetName.toLowerCase()] = ws
        }
    }
    return workbook
}

module.exports = {
  read: function (path, columns, accounts, isGlobal) {
    return new Promise((resolve, reject) => {
        console.log("::::: Reading File")
        const workbook = new Excel.Workbook();
        workbook.xlsx.readFile(path)
            .then(workbook => resolve(getFileData(workbook, columns, accounts, isGlobal)))
            .catch(e => reject(e));
    });
  },
  write: async function (path, data, accounts, columns, updateColumns) {
    console.log("::::: Writing to File")

    // open file at path
    const workbook = XLSX.readFile(path);

    // format data
    const formattedData = formatData(data, accounts, columns);

    //Add new account worksheets, if any...
    accounts.forEach(a => {
        if (!Object.keys(workbook.Sheets).map(s => s.toUpperCase()).includes(a.name.toUpperCase())) {
            createWorksheet(workbook, updateColumns, a.name)
        }
    })

    // Append new data to worksheets
    appendFile(workbook, formattedData);

    // write workbook
    XLSX.writeFile(workbook, path);
 
  }
};
