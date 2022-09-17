/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import RatingControl from '../design-system/RatingControl';

storiesOf('RatingControl', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      {story()}
    </Box>
  ))
  .add('default', () => <RatingControl />, {
    notes: {
      markdown: `
##### RatingControlStory Design Component

A design component for creating/showing numeric ratings for items..

**Resolves** [PR#127](https://github.com/Jubel-co/jp/pull/127)`,
    },
  })
  .add('with colors', () => <RatingControl color="orange" fullColor="pink" />, {
    notes: 'Change color of rating control',
  })
  .add(
    'readonly',
    () => <RatingControl color="secondary" initialRating={2} readonly={true} />,
    { notes: 'Set initial rating and readonly parameter to rating control' }
  )
  .add('fractions', () => <RatingControl fractions={2} />, {
    notes: 'Set fractions of. 1 for full icon, 2 for 1/2 icon etc.',
  })
  .add(
    'icons, sizes',
    () => (
      <RatingControl
        fractions={2}
        emptyIcon="empty-circle"
        fullIcon="full-circle"
        size={18}
      />
    ),
    { notes: 'Change icons and size of rating control' }
  );
