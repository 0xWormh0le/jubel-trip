/* eslint-disable import/first */
import React from 'react';
import { Provider } from 'mobx-react';
import { Router } from 'react-router-dom';
import { storiesOf } from '@storybook/react';
import { syncHistoryWithStore } from 'mobx-react-router';
import createBrowserHistory from 'history/createBrowserHistory';

import AppState from '../models/AppState';
import Box from '../design-system/Box';
import TripCard from '../components/TripCard';

const appState = new AppState();
const browserHistory = createBrowserHistory();
const history = syncHistoryWithStore(browserHistory, appState.routing);

appState.map.initMap({ target: null });

const trip = {
  id: 2,
  name: "Mexico's Land of the Maya",
  description:
    "It's quite hard for a trip to hit every single mark, but this region's pristine natural horizon has everything to offer. From beautiful waterfalls and rivers, hidden ruins, unique natural landmarks, azure beaches, unique food, sun-rise nightlife, and a vibrant local culture; this is truly a one-size-fits-all destination. \r\n" +
    '\r\n' +
    'Despite the Riviera Maya´s legendary reputation throughout the world, there are still many undiscovered gems that tourism forgot; explore partially excavated ruins, discover natural sinkholes that few people have ever explored, and enjoy pristine beaches that have yet to be touched by the western world. \r\n' +
    '\r\n' +
    'Head further south and delve deep into Chiapa´s lush jungles and pine forests. Witness a region only opened up to the outside world a few decades ago, where the presence of modern day Maya people bear witness to the region´s rich and uninterrupted history going back to the powerful ancient Maya civilization. Swarming with unique gastronomy, languages, and ancient traditions this is truly a cultural melting pot. For the adventurous, Chiapas is replete with amazing wildlife, rivers, turquoise waterfalls, and forgotten ruins daring the brave to go experience them. ',
  duration: 'P17D',
  estimatedCost: 2442,
  countries: ['Mexico'],
  destinations: [
    {
      id: 547,
      gid: 3519537,
      name: 'San Cristobal De Las Casas',
      continentCode: 'NA',
      countryCode: 'MX',
      continentName: 'North America',
      countryName: 'Mexico',
      latitude: 16.73176,
      longitude: -92.64126,
      duration: 'P2D',
      estimatedCost: 466,
      description:
        'San Cristóbal de las Casas has drawn in visitors for many years with its indigenous ancestry, colonial past, and comfortable cooler climate. With that being said though, this little town has attracted mostly local tourism, and lies largely undiscovered to the outside world. Situated in the Southern Mexican state of Chiapas, and nestled in a small valley surrounded by lush rolling hills and pine trees, San Cristobal de las Casa is sure to wow you.<p>The town has the largest indigenous Mayan population in Mexico, and the people of Chiapas are well-known for their weaving and crafts skills. Indeed, there are a number of iconic goods native to the area, including the colorful woven blankets, scarves, and clothing for sale in the markets, along with Chiapan coffee and pox liquor.<p>The 11 barrios (neighborhoods) are all curated for exploration, and whether the barrio is famous for cafes and restaurants, historic sites, or its markets, the streets are strikingly pedestrian friendly, making them easy to stroll around.<p>Despite being the least Catholic state in Mexico, the abundance of religious structures in the city seem to harmonize with the everlasting and charming Spanish colonial architecture of red clay tile roofs, and the archways brushed with bright colors from pink to yellow. <p>The cultural flair and quiet nature of this stunning town offers a beautiful escape from the hustle and bustle of everyday life, and San Cristobal’s prime location makes the possibilities for day trips to spectacular natural sights endless.<p>',
      shocks: {
        crime: 1,
        food: 3,
        infrastructure: 3,
        language: 2,
      },
      travelStyles: {
        oc: 0,
        an: 100,
        cn: 50,
        cu: 100,
        pp: 80,
      },
      familiarity: 'off-the-beaten-path',
      travelLogistics: 'Fly to Tuxtla Gutierrez and Drive (~1 hour)',
      airport: false,
      activities: [
        'Architecture',
        'Country Side Tours',
        'Hiking',
        'Market Visits',
        'Nature Reserves',
        'Nightlife',
        'Shopping',
        'Traditional Cuisines',
        'Waterfalls',
      ],
      naturalScenery: ['Mountains'],
      developmentLevel: 'Town',
      images: [
        {
          caption: 'San Cristobal De Las Casas',
          src:
            'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/547/shutterstock_170168309.jpg',
          thumbnail:
            'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/547/card_shutterstock_170168309.jpg',
        },
        {
          caption: 'San Cristobal De Las Casas',
          src:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/269/shutterstock_370346327.jpg',
          thumbnail:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/269/thumb_shutterstock_370346327.jpg',
        },
        {
          caption: 'San Cristobal De Las Casas',
          src:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/270/shutterstock_518526283.jpg',
          thumbnail:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/270/thumb_shutterstock_518526283.jpg',
        },
      ],
    },
    {
      id: 561,
      gid: 3522164,
      name: 'Palenque',
      continentCode: 'NA',
      countryCode: 'MX',
      continentName: 'North America',
      countryName: 'Mexico',
      latitude: 17.50953,
      longitude: -91.98248,
      duration: 'P3D',
      estimatedCost: 213,
      description:
        'Palenque is more of a feeling than a place. Here, the thick and all-encompassing fog, the silent mist rising from the trees, and the magical abundance of creatures and birds transport you into a true rainforest fairytale. <p>Cascading waterfalls and bubbling pools dot the landscape, and leaves of every shade of green hide breathtaking sites and secret spots. Saraguato monkeys trapeze their way through the towering trees, chatty parrots and bright toucans add a pop of color to the verdant landscape, and jaguars snake along the forested paths. If you’re lucky, you may even get the chance to spot one.. <p>These mystical surroundings make for a great introduction to the area’s crown jewel – its impressive Mayan ruins. This ancient city is home to some of the most spectacular and well-preserved ruins in the world, and is a true site to behold. <p>The richly creative and incomparable craftsmanship of the structures here are unsurprisingly the reason why they have been labeled as both a UNESCO World Heritage Site, and the best preserved Mayan Ruins in Mexico.  <p>This lost world, along with the beauty and silence of its surroundings, make Palenque an explorer’s paradise.<p>For an even more enchanting stay, it is worth seeking out an isolated jungle resort as the town of Palenque itself is rather uninspired. <p>',
      shocks: {
        crime: 1,
        food: 3,
        infrastructure: 4,
        language: 2,
      },
      travelStyles: {
        oc: 0,
        an: 100,
        cn: 100,
        cu: 100,
        pp: 20,
      },
      familiarity: 'off-the-beaten-path',
      travelLogistics: 'Car',
      airport: false,
      activities: [
        'Birdwatching',
        'Day Trips',
        'Hiking',
        'Historical Sites',
        'Mountain Biking',
        'Museums',
        'Nature Walks',
        'Ruins',
        'Temples',
        'Waterfalls',
        'Wildlife Sightseeing',
      ],
      naturalScenery: ['Jungle'],
      developmentLevel: 'Town',
      images: [
        {
          caption: 'Palenque',
          src:
            'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/561/shutterstock_177102716.jpg',
          thumbnail:
            'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/561/card_shutterstock_177102716.jpg',
        },
        {
          caption: 'Palenque',
          src:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/171/shutterstock_518207116__1_.jpg',
          thumbnail:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/171/thumb_shutterstock_518207116__1_.jpg',
        },
        {
          caption: 'Palenque',
          src:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/1901/shutterstock_228270220.jpg',
          thumbnail:
            'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/1901/thumb_shutterstock_228270220.jpg',
        },
      ],
    },
  ],
  activities: [
    "ATV'ing",
    'Architecture',
    'Art Galleries',
    'Artisanal Crafts',
    'Beach',
    'Biking',
    'Birdwatching',
    'Boat Trip',
  ],
  naturalScenery: ['Beach', 'Coast', 'Jungle', 'Lake', 'Mountains', 'Urban'],
  developmentLevel: ['Isolated', 'Small City', 'Town'],
  travelStyles: {
    oc: 49,
    an: 91,
    cn: 70,
    cu: 86,
    pp: 36,
  },
  shocks: {
    crime: 1,
    food: 3,
    language: 2,
    infrastructure: 3,
  },
  images: [
    {
      src:
        'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/547/shutterstock_170168309.jpg',
      thumbnail:
        'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/547/card_shutterstock_170168309.jpg',
      caption: 'San Cristobal De Las Casas',
    },
    {
      src:
        'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/561/shutterstock_177102716.jpg',
      thumbnail:
        'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/561/card_shutterstock_177102716.jpg',
      caption: 'Palenque',
    },
    {
      src:
        'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/564/shutterstock_192551102.jpg',
      thumbnail:
        'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/564/card_shutterstock_192551102.jpg',
      caption: 'Calakmul',
    },
  ],
};

storiesOf('TripCard', module)
  .addDecorator((story) => (
    <Provider app={appState}>
      <Router history={history}>
        <Box mx="auto" width={600} p={3}>
          {story()}
        </Box>
      </Router>
    </Provider>
  ))
  .add('default', () => <TripCard trip={trip} />, {
    notes: {
      markdown: `
#### Trip Card Application Component

**Resolves**
[PR#84](https://github.com/Jubel-co/jp-frontend/pull/84)`,
    },
  });
