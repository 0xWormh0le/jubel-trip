import { shallow } from 'enzyme';
import React from 'react';
import toJson from 'enzyme-to-json';

import { TravelStylesDistribution } from '../TravelStylesDistribution';
import TravelDistribution from '../../models/TravelDistribution';

const travelStyles = new TravelDistribution({
  oc: 20,
  an: 10,
  pp: 30,
  cu: 40,
  cn: 0,
});

describe('TravelStylesDistribution component', () => {
  it('renders piechart correctly', () => {
    const localProps = {
      distribution: travelStyles,
      view: 'ratings',
      t: (v) => v,
    };
    const wrapper = shallow(<TravelStylesDistribution {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  // it('renders ratings correctly', () => {
  //   const localProps = {
  //     distribution: travelStyles,
  //     view: 'ratings',
  //     t: (v) => v,
  //   };
  //   const wrapper = shallow(<TravelStylesDistribution {...localProps} />);
  //   expect(toJson(wrapper)).toMatchSnapshot();
  // });
});
