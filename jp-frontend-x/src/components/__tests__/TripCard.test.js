import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import { TripCard } from '../TripCard';

jest.mock('mapbox-gl/dist/mapbox-gl', () => ({
  Map: () => ({}),
}));

const mockMath = Object.create(global.Math);
mockMath.random = () => 0.5;
global.Math = mockMath;

const images = [
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
];

const itinerary = {
  name: 'Trip Name Goes Here',
  destinations: ['Seulla', 'Cadiz'],
  price: 1184,
  startDate: 'Jun 20, 2019',
  duration: 'P17D',
  numberOfDays: 12,
  images,
  accommodation: {
    budget: 2352,
    hostel: 1176,
    luxe: 6114,
    mid_range: 3762,
    ultra: 11758,
  },
  travelStyle: {
    oc: 20,
    an: 34,
    pp: 7,
    cu: 45,
    cn: 93,
  },
};

const accommodationsOptions = [
  { value: 'Hostel', label: 'Hostel' },
  { value: 'Budget', label: 'Budget' },
  { value: 'Mid-Range', label: 'Mid-Range' },
  { value: 'Luxe', label: 'Luxe' },
  { value: 'Ultra', label: 'Ultra' },
];

describe('TripCard component', () => {
  it('renders correctly', () => {
    const localProps = {
      trip: itinerary,
      accommodationsOptions,
      accommodation: 'Mid-Range',
      t: (v) => v,
    };
    const wrapper = shallow(<TripCard {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
