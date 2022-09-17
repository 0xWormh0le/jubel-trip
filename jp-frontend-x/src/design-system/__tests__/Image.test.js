import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import Image from '../Image';

describe('Image component', () => {
  it('should render image without crashing', () => {
    const wrapper = mount(<Image src="https://source.unsplash.com/random" />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
