const axios = require('axios');
const launches = require('./launches.mongo');
const planets = require('./planets.mongo');

const DEFAULT_FLIGHT_NUMBER = 0;

// Get all launches function
function getAllLaunches(skip, limit) {
  return launches
    .find({}, { '_id': 0, '__v': 0 })
    .sort({ flightNumber: 1 })
    .skip(skip)
    .limit(limit);
}

// Schedule new launch function and it's helpers
async function getLatestFlightNumber() {
  const latestLaunch = await launches.findOne().sort('-flightNumber');

  if (!latestLaunch) {
    return DEFAULT_FLIGHT_NUMBER;
  }

  return latestLaunch.flightNumber;
}

async function saveLaunch(launch) {
  await launches.findOneAndUpdate({
    flightNumber: launch.flightNumber,
  }, launch, {
    upsert: true,
  });
}

async function scheduleNewLaunch(launch) {
  const planet = await planets.findOne({
    keplerName: launch.target,
  });

  if (!planet) {
    throw new Error('No matching planet was found!');
  }

  const newFlightNumber = await getLatestFlightNumber() + 1;

  const newLaunch = Object.assign(launch, {
    flightNumber: newFlightNumber,
    success: true,
    upcoming: true,
    customers: ['NASA'],
  });

  await saveLaunch(newLaunch);
}

// Abort launches function and it's helpers
function findLaunch(filter) {
  return launches.findOne(filter);
}

function existsLaunchWithId(launchId) {
  return findLaunch({ flightNumber: launchId });
}


async function abortLaunchById(launchId) {
  const aborted = await launches.updateOne({
    flightNumber: launchId,
  }, {
    upcoming: false,
    success: false,
  });

  return aborted.modifiedCount === 1;
}

// Load (external API) data function and it's helpers
const SPACEX_API_URL = 'https://api.spacexdata.com/v4/launches/query';

async function populateLaunches() {
  const response = await axios.post(SPACEX_API_URL, {
    'query': {},
    'options': {
      'pagination': false,
      'populate': [
        {
          "path": "rocket",
          "select": {
            "name": 1
          }
        },
        {
          'path': 'payloads',
          'select': {
            'customers': 1
          }
        }
      ]
    }
  });

  if (response.status !== 200) {
    console.log('Problem downloading launch data!');

    throw new Error('Launch data download failed!');
  }

  const launchDocs = response.data.docs;

  for (const launchDoc of launchDocs) {
    const payloads = launchDoc['payloads'];
    const customers = payloads.flatMap((payload) => {
      return payload['customers'];
    });

    const launch = {
      flightNumber: launchDoc['flight_number'],
      mission: launchDoc['name'],
      rocket: launchDoc['rocket']['name'],
      launchDate: launchDoc['date_local'],
      upcoming: launchDoc['upcoming'],
      success: launchDoc['success'],
      customers,
    }
    console.log(launch.rocket);
    console.log(launch.customers);
    await saveLaunch(launch);
  }
}

async function loadLaunchesData() {
  const firstLaunch = await findLaunch({
    flightNumber: 1,
    rocket: 'Falcon 1',
    mission: 'FalconSat',
  });

  if (firstLaunch) {
    console.log('Launch data already loaded!');
  } else {
    await populateLaunches();
  }
}

module.exports = {
  getAllLaunches,
  scheduleNewLaunch,
  existsLaunchWithId,
  abortLaunchById,
  loadLaunchesData,
};
