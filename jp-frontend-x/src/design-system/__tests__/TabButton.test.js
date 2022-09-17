import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';

import TabButton from '../TabButton';

describe('TabButton component', () => {
  it('renders non-active TabButton correctly', () => {
    const localProps = {
      active: false,
      onClick: () => {},
      children: 'Normal Tab',
    };
    const wrapper = mount(<TabButton {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders active TabButton correctly', () => {
    const localProps = {
      active: true,
      onClick: () => {},
      children: 'Active Tab',
    };
    const wrapper = mount(<TabButton {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('triggers onClick event handler prop when button is clicked', () => {
    const localProps = {
      active: false,
      onClick: jest.fn(),
      children: 'Normal Tab',
    };
    const wrapper = mount(<TabButton {...localProps} />);
    wrapper.find('button').simulate('click');
    expect(localProps.onClick).toHaveBeenCalled();
  });
});
