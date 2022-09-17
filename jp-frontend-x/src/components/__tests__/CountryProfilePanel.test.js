import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';
import { CountryProfilePanel } from '../CountryProfilePanel';
import CountryProfile from '../CountryProfile';
import TabButton from '../../design-system/TabButton';
import TabsState from '../../models/TabsState';

jest.mock('mapbox-gl/dist/mapbox-gl', () => ({
  Map: () => ({}),
}));

const countryTabsState = new TabsState(
  [{ id: 'JP', name: 'Japan' }, { id: 'TH', name: 'Thailand' }],
  'TH'
);

describe('CountryProfilePanel component', () => {
  it('renders correctly', () => {
    const localProps = {
      isOpen: true,
      onClose: () => {},
      countryTabsState,
      t: (k, v) => v,
    };
    const wrapper = shallow(<CountryProfilePanel {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
    expect(wrapper.find(CountryProfile).prop('countryCode')).toEqual('TH');
  });

  it('switches between country tabs when tab buttons are clicked', () => {
    const localProps = {
      isOpen: true,
      onClose: () => {},
      countryTabsState,
      t: (k, v) => v,
    };
    const wrapper = shallow(<CountryProfilePanel {...localProps} />);
    expect(countryTabsState.activeTab).toEqual('TH');
    setTimeout(() => {
      wrapper
        .find(TabButton)
        .first()
        .simulate('click');
    }, 1000);
    expect(countryTabsState.activeTab).toEqual('TH');
  });
});
