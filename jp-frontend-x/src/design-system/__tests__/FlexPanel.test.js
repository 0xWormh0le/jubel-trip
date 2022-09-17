import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import FlexPanel from '../FlexPanel';

describe('FlexPanel component', () => {
  const getComponent = (props) => <FlexPanel />;
  it('should render as FlexPanel', () => {
    const component = getComponent();
    const wrapper = mount(component);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('should render centered FlexPanel', () => {
    const localProps = {
      centered: true,
    };
    const wrapper = mount(<FlexPanel {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
    expect(
      wrapper
        .find(FlexPanel)
        .first()
        .prop('centered')
    ).toEqual(true);
  });
});
