import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import RatingsAndReviews from '../RatingsAndReviews';

describe('RatingsAndReviews component', () => {
  it('renders correctly', () => {
    const localProps = {
      label: '6',
      initialRating: 3,
      fullColor: 'secondaryVariant',
      color: 'on.surface',
      ml: 3,
    };
    const wrapper = shallow(<RatingsAndReviews {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
