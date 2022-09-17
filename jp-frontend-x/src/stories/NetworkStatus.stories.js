/* eslint-disable import/first */
import React from 'react';
import { MaxPanel } from '../design-system';
import { NetworkStatus } from '../components/NetworkStatus';
import { storiesOf } from '@storybook/react';
import { withTranslation } from 'react-i18next';

const NetworkStatusT = withTranslation('common')(NetworkStatus);

storiesOf('NetworkStatus', module)
  .addDecorator((story) => <MaxPanel position="relative">{story()}</MaxPanel>)
  .add('up w/o q', () => <NetworkStatusT networkUp />)
  .add('dn w/o q', () => <NetworkStatusT nextPingTime="PT2M" />)
  .add('up w/ q', () => (
    <NetworkStatusT networkUp pendingQueries nextPingTime="PT2M" />
  ))
  .add('dn w/ q', () => <NetworkStatusT pendingQueries nextPingTime="PT2M" />)
  .add('dn w/ q cannot retry', () => (
    <NetworkStatusT canRetry={false} pendingQueries nextPingTime="PT2M" />
  ));
