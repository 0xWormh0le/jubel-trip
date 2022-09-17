import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';

import DisclosureButton from '../DisclosureButton';

describe('DisclosureButton component', () => {
  it('renders correctly', () => {
    const wrapper = mount(<DisclosureButton name="arrow-right" primary />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
