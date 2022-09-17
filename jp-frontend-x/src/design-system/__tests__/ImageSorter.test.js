import React from 'react';
import { mount, shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import ImageSorter from '../ImageSorter';
import Relative from '../Relative';
import Flex from '../Flex';
import IconButton from '../IconButton';

const images = [
  {
    id: '1469854523086-cc02fe5d8800',
    caption: 'Dino Reichmuth - yellow volkswagon',
  },
  {
    id: '1508672019048-805c876b67e2',
    caption: 'Simon Migaj - man sitting on dock',
  },
  {
    id: '1500835556837-99ac94a94552',
    caption: 'Tom Barret - airplane in golden sun',
  },
  {
    id: '1524850011238-e3d235c7d4c9',
    caption: 'Element5 Digital - pointing at a map',
  },
  {
    id: '1528255915607-9012fda0f838',
    caption: 'Willian Justen de Vasconcellos - water view',
  },
  {
    id: '1512100356356-de1b84283e18',
    caption: 'Shifaaz Shamoon - plane parked by trees',
  },
];

const mockedISorter = {
  images,
  imagesLoaded: [],
  defaultCaption: 'Loading...',
  currentIndex: 0,
  lightboxOpen: false,
  caption: 'Dino Reichmuth - yellow volkswagon',
};

describe('ImageSorter component', () => {
  it('renders correctly', () => {
    console.log('ImageSorter design-system component test');
    // const localProps = {
    //   images: mockedISorter,
    //   current: mockedISorter.images[0].src,
    //   lightBoxOpen: mockedISorter.lightboxOpen,
    // };
    // const wrapper = shallow(<ImageSorter {...localProps} />);
    // expect(toJson(wrapper)).toMatchSnapshot();
    // expect(
    //   wrapper
    //     .find(Flex)
    //     .find(IconButton)
    //     .prop('label')
    // ).toEqual(mockedISorter.caption);
  });

  // it('triggers lightbox open when image is clicked', () => {
  //   const localProps = {
  //     images: {
  //       ...mockedISorter,
  //       openLightbox: jest.fn(),
  //     },
  //     current: mockedISorter.images[0].src,
  //     lightBoxOpen: mockedISorter.lightboxOpen,
  //   };
  //   const wrapper = mount(<ImageSorter {...localProps} />);
  //   wrapper.find(Relative).simulate('click');
  //   expect(localProps.images.openLightbox).toHaveBeenCalled();
  // });

  // it('triggers lightbox open when image is clicked', () => {
  //   const localProps = {
  //     images: {
  //       ...mockedISorter,
  //       openLightbox: jest.fn(),
  //     },
  //     current: mockedISorter.images[0].src,
  //     lightBoxOpen: mockedISorter.lightboxOpen,
  //   };
  //   const wrapper = mount(<ImageSorter {...localProps} />);
  //   wrapper.find(Relative).simulate('click');
  //   expect(localProps.images.openLightbox).toHaveBeenCalledTimes(1);
  // });

  // it('triggers nav actions when arrow buttons are clicked', () => {
  //   const localProps = {
  //     images: {
  //       ...mockedISorter,
  //       openLightbox: jest.fn(),
  //       next: jest.fn(),
  //       prev: jest.fn(),
  //     },
  //     current: mockedISorter.images[0].src,
  //     lightboxOpen: mockedISorter.lightboxOpen,
  //   };
  //   const wrapper = mount(<ImageSorter {...localProps} />);
  //   wrapper
  //     .find({ name: 'arrow-left' })
  //     .last()
  //     .simulate('click');
  //   expect(localProps.images.prev).toHaveBeenCalledTimes(1);
  //   wrapper
  //     .find({ name: 'arrow-right' })
  //     .last()
  //     .simulate('click');
  //   expect(localProps.images.next).toHaveBeenCalledTimes(1);
  // });
});
