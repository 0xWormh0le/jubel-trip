import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import Icon from '../Icon';

describe('Icon component', () => {
  it('should render icon without crashing', () => {
    const wrapper = mount(<Icon name="menu" />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('should render icon with props', () => {
    const props = {
      name: 'menu',
    };
    const component = mount(<Icon {...props} />);
    expect(component.prop('name')).toBe('menu');
  });
});
