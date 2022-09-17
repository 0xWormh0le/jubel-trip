import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import { shallowWithTheme } from '../../utils/testHelpers';
import DonutChart from '../DonutChart';

let colors = ['#69a6f1', '#64e5aa', '#cea0f8', '#f1df86'];
let props;
let component;
let travelStyles;
describe('DonutChart component', () => {
  beforeEach(() => {
    travelStyles = [
      {
        color: '#69a6f1',
        label: 'OC',
        name: 'Oceanist',
        onColor: '#ffffff',
        x: 'oc',
        y: 10.050251256281408,
      },
      {
        color: '#64e5aa',
        label: 'AN',
        name: 'Active Naturist',
        onColor: '#ffffff',
        x: 'an',
        y: 17.08542713567839,
      },
      {
        color: '#a3f490',
        label: 'CN',
        name: 'Chilled Naturist',
        onColor: '#ffffff',
        x: 'cn',
        y: 46.733668341708544,
      },
      {
        color: '#cea0f8',
        label: 'CU',
        name: 'Culturist',
        onColor: '#ffffff',
        x: 'cu',
        y: 22.613065326633166,
      },
      {
        color: '#f1df86',
        label: 'PP',
        name: 'Party Purist',
        onColor: '#ffffff',
        x: 'pp',
        y: 3.5175879396984926,
      },
    ];
  });
  it('should always render a DonutChart', () => {
    const localProps = {
      data: travelStyles,
      colors,
      view: 'pie',
    };
    const wrapper = shallowWithTheme(<DonutChart {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
