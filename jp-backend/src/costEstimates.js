import xlsx from 'xlsx';
import https from 'https';
import fs from 'fs';
import { groupBy, values, map, sum, startCase, lowerCase } from 'lodash';
const sqlite3 = require('sqlite3').verbose();

const db = new sqlite3.Database('./jubel.db');

const US_DOS_PD_COSTS_BASE_URL = 'https://aoprals.state.gov/content/Documents/';
const monthNames = ["January", "February", "March", "April", "May","June",
  "July", "August", "September", "October", "November","December"];
const date = new Date();
const month = monthNames[date.getMonth()];
const year = date.getFullYear();
const file = fs.createWriteStream(`${month+year}.xlsx`);

/**
 * Fetches data from US Dept of State's Office of Allowances
 * @returns {Promise<string>}
 */
const getNewData = () => new Promise((resolve, reject) => {
  https.get(US_DOS_PD_COSTS_BASE_URL+month+year+'PD.xls', res => {
    res.pipe(file);
    file.on('finish', () =>
      file.close(() => resolve(file.path))
    );
    file.on('error', () => reject());
  });
});

const parseData = (fileName) => new Promise((res, _) => {
  const workbook = xlsx.readFile(fileName);
  const sheetNameList = workbook.SheetNames;
  const json = xlsx.utils.sheet_to_json(workbook.Sheets[sheetNameList[0]]);
  const averaged = map(
    values(groupBy(json, (entry => `${entry['Country']}--${entry['Location ']}`))),
    arr => arr.length > 1 ? { ...arr[0], 'Per Diem ': sum(arr.map(x => x['Per Diem ']))/arr.length } : arr[0]
  );
  averaged.forEach(v => {
    const name = lowerCase(v['Location ']);
    const country = lowerCase(v['Country']);
    const perDiem = v['Per Diem '];
    db.exec(`UPDATE cost_estimates SET per_diem = ${perDiem} WHERE name = "${name}" AND country = "${country}";
      INSERT INTO cost_estimates (name, country, per_diem) SELECT "${name}", "${country}", ${perDiem} 
      WHERE (Select Changes() = 0)`,
      (err, ok) => err ? console.warn(err) : null
    )
  });
  return res(true);
});

getNewData()
  .then(res => parseData(res))
  .catch(err => console.error('Error fetching new data and writing to file', err));
