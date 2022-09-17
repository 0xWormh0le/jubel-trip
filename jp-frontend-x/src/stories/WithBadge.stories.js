/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import Flex from '../design-system/Flex';
import IconButton from '../design-system/IconButton';
import Relative from '../design-system/Relative';
import WithBadge from '../design-system/WithBadge';

const markdown = `
#### WithBadge Design Component

A design component use badge in left or right top side. Here is Icon with badge.

**Resolves**
[PR#109](https://github.com/Jubel-co/jp/pull/109)`;

storiesOf('WithBadge', module)
  .addDecorator((story) => (
    <Box mx="auto" width={600} p={3}>
      {story()}
    </Box>
  ))
  .add(
    'as children prop',
    () => (
      <WithBadge bagdeLabel="3">
        <IconButton name="my-trips" />
      </WithBadge>
    ),
    { notes: { markdown } }
  )
  .add(
    'as sibling',
    () => (
      <Relative p={3}>
        <WithBadge bagdeLabel="3" position="right" />
        <Flex alignItems="flex-end" justifyContent="flex-end">
          <IconButton name="my-trips" />
        </Flex>
      </Relative>
    ),
    { notes: { markdown } }
  );
