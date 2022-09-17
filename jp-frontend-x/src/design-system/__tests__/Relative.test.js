import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import Relative from '../Relative';

describe('Relative component', () => {
  it('should render as Relative', () => {
    const wrapper = mount(<Relative />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
