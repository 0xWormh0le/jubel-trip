/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import ReviewButton from '../design-system/ReviewButton';

storiesOf('ReviewButton', module)
  .addDecorator((story) => (
    <Box mx="auto" width={500} p={3}>
      {story()}
    </Box>
  ))
  .add('default', () => <ReviewButton label="Comments" />, {
    notes: {
      markdown: `
#### ReviewButton Design Component

A design component to act as a button to reveal hidden comments
and reviews.

**Resolves** [IS#90](https://github.com/Jubel-co/jp/issues/90) /
              [PR#110](https://github.com/Jubel-co/jp/pull/110)`,
    },
  })
  .add('with custom color', () => (
    <ReviewButton label="Green Comments" color="green" />
  ));
