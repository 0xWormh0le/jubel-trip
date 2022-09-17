/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import RatingsAndReviews from '../design-system/RatingsAndReviews';

storiesOf('RatingsAndReviews', module)
  .addDecorator((story) => (
    <Box mx="auto" width={500} p={3}>
      {story()}
    </Box>
  ))
  .add(
    'default',
    () => (
      <RatingsAndReviews
        label="6"
        initialRating={3}
        fullColor="secondaryVariant"
        color="on.surface"
        ml={3}
      />
    ),
    {
      notes: {
        markdown: `
#### RatingsAndReviews Design Component

A design component to act as a combination of rating and review components.

**Resolves**  [PR#41](https://github.com/Jubel-co/jp-frontend/pull/41)`,
      },
    }
  );
