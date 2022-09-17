import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';
import React from 'react';

import { DestinationAlternatives } from '../DestinationAlternatives';

jest.mock('mapbox-gl/dist/mapbox-gl', () => ({
  Map: () => ({}),
}));
const alternatives = [
  {
    id: 'sydney',
    name: 'Sydney',
    image: 'https://source.unsplash.com//800x600/?sydney',
  },
  {
    id: 'melbourne',
    name: 'Melbourne',
    image: 'https://source.unsplash.com/1600x900/?melbourne',
  },
  {
    id: 'adelaide',
    name: 'Adelaide',
    image: 'https://source.unsplash.com/1600x900/?adelaide',
  },
  {
    id: 'newcastle',
    name: 'Newcastle',
    image: 'https://source.unsplash.com/1600x900/?newcastle',
  },
  {
    id: 'gold_coast',
    name: 'Gold Coast',
    image: 'https://source.unsplash.com/1600x900/?gold-coast',
  },
  {
    id: 'canberra',
    name: 'Canberra',
    image: 'https://source.unsplash.com/1600x900/?canberra',
  },
];

describe('DestinationAlternatives component', () => {
  it('renders DestinationAlternatives correctly', () => {
    const localProps = {
      alternatives,
    };
    const wrapper = shallow(<DestinationAlternatives {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
