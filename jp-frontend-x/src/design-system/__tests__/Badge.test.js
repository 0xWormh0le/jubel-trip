import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import Badge from '../Badge';

describe('Bagde component', () => {
  it('should render as badge', () => {
    const wrapper = mount(<Badge />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
