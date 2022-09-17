import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import Flex from '../Flex';

describe('Flex component', () => {
  it('should render as Flex', () => {
    const wrapper = mount(<Flex />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('should render centered Flex', () => {
    const localProps = {
      centered: true,
    };
    const wrapper = mount(<Flex {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
    expect(
      wrapper
        .find(Flex)
        .first()
        .prop('centered')
    ).toEqual(true);
  });
});
