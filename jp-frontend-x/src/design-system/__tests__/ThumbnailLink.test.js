import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import ThumbnailLink from '../ThumbnailLink';
import { mountWithTheme } from '../../utils/testHelpers';

describe('ThumbnailLink component', () => {
  it('should render as ThumbnailLink', () => {
    const wrapper = mountWithTheme(
      <ThumbnailLink
        width={200}
        height={125}
        label="Oceanist"
        goTo={`/travel-style/oc`}
        image="https://source.unsplash.com/random"
      />
    );
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
