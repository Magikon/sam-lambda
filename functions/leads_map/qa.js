const map = require("./map.js");
const path = require('path');

const fs = require('fs');

const yargs = require('yargs');

const argv = yargs(process.argv.slice(2))
  .example('$0 -i input_file.json -o output_file.json', 'Set input and output files in current directory')
  .example('$0 -i /data/input/input_file.json -o /data/output/output_file.json --absolute_paths', 'Set input and output files with absolute paths')

  .alias('i', 'input')
  .nargs('i', 1)
  .describe('i', 'Input File')
  .alias('o', 'output')
  .nargs('o', 1)
  .describe('o', 'Output File')
  .demandOption(['i', 'o'])
  .string(['i', 'o'])
  .option('simple_output', {
    default: false,
    type: 'boolean',
    description: 'Output a simple event'
  })
  .option('absolute_paths', {
    default: false,
    type: 'boolean',
    description: 'Registers input and output values as absolute paths'
  })
  .help('h')
  .alias('h', 'help')
  .argv;

const inputPath = argv.absolute_paths ? argv.input : path.join(__dirname, argv.input);
const outputFile = argv.output

// Check if output file is in json format
if (outputFile.split('.')[outputFile.split('.').length - 1] !== 'json') throw `Invalid output file: ${outputFile}. expected json - <file_name>.json`

const outputPath = argv.absolute_paths ? argv.output : path.join(__dirname, outputFile)

if (fs.existsSync(inputPath)) {
  let event = fs.readFileSync(inputPath);
  event = JSON.parse(event)
  const customer = event.customer
  const account_config = event.accountConfig
  const lead_id = event.lead.id

  console.log('CUSTOMER:: ', customer)
  console.log('Lead ID:: ', lead_id)
  console.log('Input File: ', inputPath)

  // Setting isQA flag
  event.isQA = true;

  map.handler(event, {}, (a, data) => {
    if (data.error) {
      console.log("ERROR; ", data.error.message || data.error)
    } else if (data.body.map !== null) {
      let result = {}

      // Output a Recommend ready json file or a simple one
      if (argv.simple_output) {
        result = data.body.map
      } else {
        result = {
          customer,
          map: data.body.map,
          availability: null,
          recommendation: null,
          probabilities: null,
          account_config,
          existingLeadPayload: null,
          lead_id,
          active_prospect: false
        }
      }

      fs.writeFileSync(outputPath, JSON.stringify(result))
      console.log('Output:: ', outputPath)
    }
  })

} else {
  throw `File Path: ${inputPath} does not exist! Please enter a valid input file in the same directory`
}