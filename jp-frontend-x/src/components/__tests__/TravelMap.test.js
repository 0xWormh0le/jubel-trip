import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import { TravelMap } from '../TravelMap';

jest.mock('mapbox-gl/dist/mapbox-gl', () => ({
  Map: () => ({}),
}));

const geoJsonMock = {
  points: {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        geometry: {
          type: 'Point',
          coordinates: [-20, 30],
        },
        properties: {
          tripId: 1,
          number: 1,
          place: 'Trip 1',
        },
      },
      {
        type: 'Feature',
        geometry: {
          type: 'Point',
          coordinates: [-22, 29],
        },
        properties: {
          tripId: 2,
          number: 2,
          place: 'Trip 2',
        },
      },
    ],
  },
};

const propsMock = {
  wantsControls: false,
  viewport: {
    width: 1080,
    height: 768,
    zoom: 2,
    longitude: -40,
    latitude: 20,
    pitch: 0,
    bearing: 30,
    transitionDuration: 1500,
  },
  onViewportChange: () => {},
  satOn: true,
  toggleSatStyle: () => {},
  onLoad: () => {},
  mapStyle: '',
  geoJson: geoJsonMock,
  t: (k, v) => v,
};

describe('TravelMap component', () => {
  it('renders correctly', () => {
    const localProps = propsMock;
    const wrapper = shallow(<TravelMap {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders map controls if `wantsControls` is specified', () => {
    const localProps = {
      ...propsMock,
      wantsControls: true,
    };
    const wrapper = shallow(<TravelMap {...localProps} />);
    // expect(wrapper.find(NavigationControl)).toHaveLength(1);
    // expect(wrapper.find('.TravelMap__satellite-ctrl')).toHaveLength(1);
  });
});
