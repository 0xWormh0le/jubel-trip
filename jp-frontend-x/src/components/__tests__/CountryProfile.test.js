import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import { CountryProfile } from '../CountryProfile';

jest.mock('mapbox-gl/dist/mapbox-gl', () => ({
  Map: () => ({}),
}));

const country = {
  countryCode: 'JP',
  name: 'Japan',
  description:
    'Japan is distinct - no culture has charmed the world quite like Japanese culture.',
  images: [
    {
      caption: 'Tokyo',
      src:
        'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/28/shutterstock_175851101.jpg',
      thumbnail:
        'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/28/card_shutterstock_175851101.jpg',
    },
    {
      caption: 'Tokyo',
      src:
        'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/969/shutterstock_268422755.jpg',
      thumbnail:
        'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/969/thumb_shutterstock_268422755.jpg',
    },
  ],
};

describe('CountryProfile component', () => {
  it('renders correctly', () => {
    const localProps = {
      country,
      t: (k, v) => v,
    };
    const wrapper = shallow(<CountryProfile {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
