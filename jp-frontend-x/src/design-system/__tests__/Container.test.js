import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';

import Container from '../Container';

describe('Container component', () => {
  it('should render Container component', () => {
    const wrapper = mount(<Container />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
