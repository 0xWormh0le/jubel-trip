import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import MaxPanel from '../MaxPanel';

describe('MaxPanel component', () => {
  it('should render as MaxPanel', () => {
    const wrapper = mount(<MaxPanel />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
