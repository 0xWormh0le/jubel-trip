import React from 'react';
import toJson from 'enzyme-to-json';
import { mountWithTheme } from '../../utils/testHelpers';
import Hr from '../Hr';

describe('Hr component', () => {
  it('should render as Hr', () => {
    const wrapper = mountWithTheme(<Hr />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
