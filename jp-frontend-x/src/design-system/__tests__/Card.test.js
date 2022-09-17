import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import Card from '../Card';

describe('Card component', () => {
  it('should render as card', () => {
    const wrapper = mount(<Card />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
