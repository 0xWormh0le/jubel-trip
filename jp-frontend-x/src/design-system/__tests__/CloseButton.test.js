import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import CloseButton from '../CloseButton';

describe('CloseButton component', () => {
  it('should render as CloseButton', () => {
    const wrapper = mount(<CloseButton />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('simulates click events', () => {
    const localProps = {
      onClick: () => {},
    };
    const wrapper = mount(<CloseButton {...localProps} />);
    wrapper.find(CloseButton).simulate('click');
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('should render disabled CloseButton', () => {
    const localProps = {
      disabled: true,
    };
    const wrapper = mount(<CloseButton {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
    expect(
      wrapper
        .find(CloseButton)
        .first()
        .prop('disabled')
    ).toEqual(true);
  });
});
