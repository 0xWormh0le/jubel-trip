import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import Alternatives from '../Alternatives';

describe('Alternatives component', () => {
  it('should render alternatives component', () => {
    const props = {
      name: 'Alternative',
      url: 'https://source.unsplash.com/random',
    };
    const wrapper = mount(<Alternatives {...props} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
