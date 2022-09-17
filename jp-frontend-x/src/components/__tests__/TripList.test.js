import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import { TripList } from '../TripList';

jest.mock('mapbox-gl/dist/mapbox-gl', () => ({
  Map: () => ({}),
}));

const trips = [
  {
    id: 564,
    gid: 6942866,
    name: 'Calakmul',
    continentCode: 'NA',
    countryCode: 'MX',
    continentName: 'North America',
    countryName: 'Mexico',
    latitude: 18,
    longitude: -89.75,
    duration: 'P1D',
    estimatedCost: 194,
    description: `Calakmul is an unspoiled tropical reserve that signifies "two adjacent mounds" in Mayan. Its impressive ruins are rightfully recognized by UNESCO as a Cultural Heritage of Humanity, and its pristine forest is known to be the second lung of America. <p>You'll easily be able to spot many of the over 86 species of mammals. Enjoy the excitement of being close to jaguars, pumas, ocelots, jaguarundis, and margays, (five of the six feline species currently living in México), as well as spider monkeys, anteaters, eagles, tapirs, and more than 300 species of birds. Admire the beauty of the orchids: you will find about 73 different types here. <p>Finally, you simply CANNOT miss the Calakmul archaeological site, it is an absolute hidden gem. Discovering the settlements of this Mayan urban center lost within the jungle and swallowed by its vegetation is an experience worthy of a thorough visit.  `,
    shocks: {
      crime: 0,
      food: 3,
      infrastructure: 5,
      language: 3,
    },
    travelStyles: {
      oc: 0,
      an: 100,
      cn: 80,
      cu: 100,
      pp: 0,
    },
    conventionality: 'off-the-grid',
    travelLogistics: 'Car',
    airport: false,
    activities: [
      'Hiking',
      'Museums',
      'Nature Reserves',
      'Ruins',
      'Wildlife Sightseeing',
    ],
    naturalScenery: ['Jungle'],
    developmentLevel: 'Isolated',
    images: [
      {
        caption: 'Calakmul',
        src:
          'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/564/shutterstock_192551102.jpg',
        thumbnail:
          'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/564/card_shutterstock_192551102.jpg',
      },
      {
        caption: 'Calakmul',
        src:
          'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/1539/shutterstock_194179301.jpg',
        thumbnail:
          'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/1539/thumb_shutterstock_194179301.jpg',
      },
      {
        caption: 'Calakmul',
        src:
          'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/1540/shutterstock_562749169.jpg',
        thumbnail:
          'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/1540/thumb_shutterstock_562749169.jpg',
      },
    ],
  },
  {
    id: 520,
    gid: 3532465,
    name: 'Bacalar',
    continentCode: 'NA',
    countryCode: 'MX',
    continentName: 'North America',
    countryName: 'Mexico',
    latitude: 18.68109,
    longitude: -88.39333,
    duration: 'P3D',
    estimatedCost: 186,
    description:
      "Bacalar is a true natural treasure. The Lake of Seven Colors is a freshwater paradise that at first glance resembles the turquoise waters and white sands of the Caribbean. However, upon first swimming in its water, you'll realize this place is a category of its own. <p>From afar, the lake amazes with beautiful turquoise-hued waters, but up and close, they turn crystal clear which makes them perfect for cave diving, snorkeling, or simply floating around. Thanks to the freshwater springs feeding the lake, you'll come out of its waters feeling energized, clean, and fresh, so don't be surprised if you end up taking a dip over and over again!<p>Bacalar's lake is also home to some of the oldest organisms on the planet called stromatolites, which could help decipher the origin of life, and also the secrets left by pirates who hid there in the 17th and 18th centuries.<p>Beyond the natural beauty of the lake, Bacalar is simply a heavenly town that will make you fall in love with its small fishermen’s houses, delicious seafood, and the nearby Mayan ruins waiting for you to go out and explore them.",
    shocks: {
      crime: 1,
      food: 2,
      infrastructure: 3,
      language: 2,
    },
    travelStyles: {
      oc: 100,
      an: 100,
      cn: 100,
      cu: 80,
      pp: 20,
    },
    conventionality: 'off-the-beaten-path',
    travelLogistics: 'Car',
    airport: false,
    activities: [
      'Beach',
      'Day Trips',
      'Kayaking',
      'Ruins',
      'Sailing',
      'Scuba Diving',
      'Snorkeling',
      'Swimming',
    ],
    naturalScenery: ['Jungle', 'Lake'],
    developmentLevel: 'Town',
    images: [
      {
        caption: 'Bacalar',
        src:
          'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/520/shutterstock_249963496.jpg',
        thumbnail:
          'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/520/card_shutterstock_249963496.jpg',
      },
      {
        caption: 'Bacalar',
        src:
          'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/244/shutterstock_1139270006.jpg',
        thumbnail:
          'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/244/thumb_shutterstock_1139270006.jpg',
      },
      {
        caption: 'Bacalar',
        src:
          'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/245/shutterstock_632325497.jpg',
        thumbnail:
          'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/245/thumb_shutterstock_632325497.jpg',
      },
    ],
  },
];

describe('TripCard component', () => {
  it('renders correctly', () => {
    const localProps = {
      trips,
      t: (v) => v,
    };
    const wrapper = shallow(<TripList {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
