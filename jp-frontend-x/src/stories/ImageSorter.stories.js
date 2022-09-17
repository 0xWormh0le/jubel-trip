/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import ImageSorter from '../design-system/ImageSorter';

const images = [
  {
    caption: 'San Cristobal De Las Casas',
    src:
      'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/547/shutterstock_170168309.jpg',
    thumbnail:
      'https://jubelproduction.s3.amazonaws.com/uploads/destination/main_image/547/card_shutterstock_170168309.jpg',
  },
  {
    caption: 'San Cristobal De Las Casas',
    src:
      'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/269/shutterstock_370346327.jpg',
    thumbnail:
      'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/269/thumb_shutterstock_370346327.jpg',
  },
  {
    caption: 'San Cristobal De Las Casas',
    src:
      'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/270/shutterstock_518526283.jpg',
    thumbnail:
      'https://jubelproduction.s3.amazonaws.com/uploads/photo/image/270/thumb_shutterstock_518526283.jpg',
  },
];

storiesOf('ImageSorter', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      {story()}
    </Box>
  ))
  .add(
    'default',
    () => (
      <ImageSorter
        height={300}
        minHeight={300}
        borderRadius="none"
        images={images}
      />
    ),
    {
      notes: {
        markdown: `
##### ImageSorter Design Component

A design component to facilitate paging through
thumbnail images easily.

**Resolves**
[PR#107](https://github.com/Jubel-co/jp/pull/107)`,
      },
    }
  );
