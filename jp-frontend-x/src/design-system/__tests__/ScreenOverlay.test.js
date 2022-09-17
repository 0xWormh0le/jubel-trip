import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import ScreenOverlay from '../ScreenOverlay';

describe('ScreenOverlay component', () => {
  it('should render as ScreenOverlay', () => {
    const wrapper = mount(<ScreenOverlay />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
