import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import SecondaryButton from '../SecondaryButton';

describe('SecondaryButton component', () => {
  const getComponent = (props) => <SecondaryButton />;
  it('should render as SecondaryButton', () => {
    const component = getComponent();
    const wrapper = mount(component);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
