import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import HoverText from '../HoverText';

describe('HoverText component', () => {
  it('should render as HoverText', () => {
    const wrapper = mount(<HoverText>Text</HoverText>);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
