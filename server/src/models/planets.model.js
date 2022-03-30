const { parse } = require('csv-parse');
const fs = require('fs');
const path = require('path');
const planets = require('./planets.mongo');

const ColumnsNames = {
  KOI_DISPITION: 'koi_disposition',
  KOI_INSOL: 'koi_insol',
  KOI_PRAD: 'koi_prad',
  KEPLER_NAME: 'kepler_name',
};

// Criteria for a plant to be habitable
const DISPOITION = 'CONFIRMED';
const MIN_INSOLATION_FLUX = 0.36;
const MAX_INSOLATION_FLUX = 1.11;
const MAX_RADIUS = 1.6;

// Check a planet's habitability
const isHabitablePlanet = planet => {
  return planet[ColumnsNames.KOI_DISPITION] === DISPOITION
    && planet[ColumnsNames.KOI_INSOL] > MIN_INSOLATION_FLUX && planet[ColumnsNames.KOI_INSOL] < MAX_INSOLATION_FLUX
    && planet[ColumnsNames.KOI_PRAD] < MAX_RADIUS;
};

function loadPlanetsData() {
  return new Promise((resolve, reject) => {
  // Read file, parse and filter data
    fs.createReadStream(path.join('data', 'kepler_data.csv'))
      .pipe(parse({
        comment: '#',
        columns: true,
      }))
      .on('data', async (data) => {
        if (isHabitablePlanet(data)) {
          await savePlanets(data);
        }
      })
      .on('error', (err) => {
        console.log(err);
        reject(err);
      })
      .on('end', async () => {
        const countPlanetsFound = (await getAllPlanets()).length;

        console.log(`${countPlanetsFound} habitable planets found!`);

        resolve();
      });
  })
}

async function savePlanets(planet) {
  try {
    await planets.updateOne({
      keplerName: planet.kepler_name,
    }, {
      keplerName: planet.kepler_name,
    }, {
      upsert: true,
    });
  } catch (err) {
    console.log(`Could not save planet ${err}`);
  }
}

function getAllPlanets() {
  return planets.find({}, {
    '_id': 0,
    '__v': 0,
  });
}

module.exports = {
  loadPlanetsData,
  getAllPlanets,
};