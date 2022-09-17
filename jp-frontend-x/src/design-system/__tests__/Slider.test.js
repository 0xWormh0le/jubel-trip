import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import Slider, { StyledSlider, Label as SliderLabel } from '../Slider';
import Text from '../Text';

describe('Slider component', () => {
  it('renders horizontal slider correctly', () => {
    const localProps = {
      name: 'horizSlider',
      label: 'Horizontal Slider',
      minLabel: 'Min Value',
      maxLabel: 'Max Value',
      value: 30,
      onChange: () => {},
    };
    const wrapper = shallow(<Slider {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
    // test labels rendered
    expect(wrapper.find(Text).prop('children')).toEqual(localProps.label);
    expect(
      wrapper
        .find(SliderLabel)
        .at(0)
        .prop('children')
    ).toEqual(localProps.minLabel);
    expect(
      wrapper
        .find(SliderLabel)
        .at(1)
        .prop('children')
    ).toEqual(localProps.maxLabel);
  });

  it('renders horizontal slider correctly in rtl mode', () => {
    const localProps = {
      rtl: true,
      name: 'rtlSlider',
      label: 'RTL Horizontal Slider',
      minLabel: 'Min Value',
      maxLabel: 'Max Value',
      value: 30,
      onChange: () => {},
    };
    const wrapper = shallow(<Slider {...localProps} />);

    expect(toJson(wrapper)).toMatchSnapshot();
    // test labels rendered
    expect(wrapper.find(Text).prop('children')).toEqual(localProps.label);
    expect(
      wrapper
        .find(SliderLabel)
        .at(0)
        .prop('children')
    ).toEqual(localProps.maxLabel);
    expect(
      wrapper
        .find(SliderLabel)
        .at(1)
        .prop('children')
    ).toEqual(localProps.minLabel);
  });

  it('renders vertical slider correctly', () => {
    const localProps = {
      vertical: true,
      minLabel: 'minLabel',
      maxLabel: 'maxLabel',
      height: 150,
      spacing: 4,
      thickness: 1,
      handleShape: 'rect',
      name: 'mixer.an',
      label: 'AN',
      value: 30,
      onChange: () => {},
    };
    const wrapper = shallow(<Slider {...localProps} />);

    expect(toJson(wrapper)).toMatchSnapshot();
    // test labels rendered
    expect(wrapper.find(Text).prop('children')).toEqual(localProps.label);
    expect(
      wrapper
        .find(SliderLabel)
        .at(0)
        .prop('children')
    ).toEqual(localProps.minLabel);
    expect(
      wrapper
        .find(SliderLabel)
        .at(1)
        .prop('children')
    ).toEqual(localProps.maxLabel);
  });

  it('sets the minimum and maximum value range correctly', () => {
    const localProps = {
      min: 30,
      max: 60,
      minLabel: 'minLabel',
      maxLabel: 'maxLabel',
      height: 150,
      spacing: 4,
      thickness: 1,
      handleShape: 'rect',
      name: 'mixer.an',
      label: 'AN',
      value: 30,
      onChange: () => {},
    };
    const wrapper = shallow(<Slider {...localProps} />);

    expect(wrapper.find(StyledSlider).prop('domain')).toEqual([
      localProps.min,
      localProps.max,
    ]);
  });

  it('triggers `onChange` function prop when dragging is ended', () => {
    const localProps = {
      vertical: true,
      minLabel: 'minLabel',
      maxLabel: 'maxLabel',
      height: 150,
      spacing: 4,
      thickness: 1,
      handleShape: 'rect',
      name: 'testOnChangeSlider',
      label: 'AN',
      value: 30,
      onChange: jest.fn(),
    };
    const wrapper = shallow(<Slider {...localProps} />);

    wrapper.find(StyledSlider).prop('onSlideEnd')(33);
    expect(localProps.onChange).toHaveBeenCalledWith('testOnChangeSlider', 33);
  });
});
