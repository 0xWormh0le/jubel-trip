import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import HoverBox from '../HoverBox';

describe('HoverBox component', () => {
  it('should render as HoverBox', () => {
    const wrapper = mount(<HoverBox />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
