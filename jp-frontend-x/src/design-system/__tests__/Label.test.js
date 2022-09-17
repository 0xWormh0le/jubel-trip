import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';

import Label from '../Label';

describe('Label component', () => {
  it('should render Label component', () => {
    const wrapper = mount(<Label>Label</Label>);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('should render accessiblyHide Label', () => {
    const localProps = {
      hidden: true,
    };
    const wrapper = mount(<Label {...localProps}>Hidden Label</Label>);
    expect(toJson(wrapper)).toMatchSnapshot();
    expect(
      wrapper
        .find(Label)
        .first()
        .prop('hidden')
    ).toEqual(true);
    expect(
      wrapper
        .find(Label)
        .first()
        .prop('as')
    ).toBe('label');
    expect(wrapper.length).toBe(1);
  });
});
