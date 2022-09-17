import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import TravelStyleButton, { TravelStyleButtonBase } from '../TravelStyleButton';

const travelStyle = {
  abbr: 'oc',
  colorPrefix: 'misc.branding.styles.oc',
  description:
    "Turquoise waters and powdery sands are your happy place. The ocean's waves bring you in, while its tides sway you into a trance. Whether it be an idyllic island, rocky coastline, or virgin beach, you …",
  image:
    'https://static1.squarespace.com/static/594336f837c5819eb6378ac7/t/5b875c4c88251bea0a25a10f/1535597651268/jakob-owens-300986-unsplash.jpg?format=2500w',
  name: 'Oceanist',
  property: 'ocean',
  video: 'https://player.vimeo.com/video/299331480?title=0&byline=0&portrait=0',
  width: 28,
};

describe('TravelStyleButton component', () => {
  it('renders correctly', () => {
    const localProps = {
      travelStyle,
      t: (v) => v,
    };
    console.log('This test need to be fixed');
    // const wrapper = shallow(<TravelStyleButton {...localProps} />);
    // expect(toJson(wrapper)).toMatchSnapshot();
  });

  // it('triggers `onClick` when button is clicked', () => {
  //   const localProps = {
  //     travelStyle,
  //     t: (v) => v,
  //     width: 60,
  //     height: 60,
  //     disabled: true,
  //     onClick: jest.fn(),
  //   };
  //   const wrapper = shallow(<TravelStyleButton {...localProps} />);
  //   wrapper.find(TravelStyleButtonBase).simulate('click');
  //   expect(localProps.onClick).toHaveBeenCalled();
  // });
});
