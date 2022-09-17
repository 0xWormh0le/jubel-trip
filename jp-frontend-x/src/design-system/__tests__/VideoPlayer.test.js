import React from 'react';
import { shallow } from 'enzyme';
import ReactPlayer from 'react-player';
import toJson from 'enzyme-to-json';

import VideoPlayer from '../VideoPlayer';

describe('VideoPlayer component', () => {
  it('renders Video player correctly', () => {
    const localProps = {
      src: 'https://www.youtube.com/embed/0LuiUtv2kug',
      height: 320,
      width: 480,
    };
    const wrapper = shallow(<VideoPlayer {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('passes video URL correctly', () => {
    const localProps = {
      src: 'https://www.youtube.com/embed/0LuiUtv2kug',
      height: 320,
      width: 480,
    };
    const wrapper = shallow(<VideoPlayer {...localProps} />);
    expect(wrapper.find(ReactPlayer).prop('url')).toBe(localProps.src);
  });

  it('plays automatically if autoPlay is specified', () => {
    const localProps = {
      autoPlay: true,
      src: 'https://www.youtube.com/embed/0LuiUtv2kug',
      height: 320,
      width: 480,
    };
    const wrapper = shallow(<VideoPlayer {...localProps} />);
    expect(wrapper.find(ReactPlayer).prop('playing')).toBe(true);
  });
});
