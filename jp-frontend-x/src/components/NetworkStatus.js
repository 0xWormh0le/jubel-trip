import React from 'react';
import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import { withTranslation } from 'react-i18next';

import { Flex, Icon, Text, Button } from '../design-system';

export const NetworkStatus = ({
  networkUp,
  pendingQueries,
  nextPingTime,
  checkAgain,
  canRetry = true,
  t,
}) =>
  networkUp && !pendingQueries ? null : (
    <Flex
      alignItems="center"
      justifyContent="space-between"
      boxShadow={1}
      borderRadius="sm"
      p={1}
      pl={2}
      mx={2}
      my={1}
      overflow="hidden"
      bg={networkUp ? 'success' : 'error'}
      color={networkUp ? 'on.success' : 'on.error'}
      onClick={!networkUp && checkAgain ? () => checkAgain() : undefined}
      cursor={!networkUp ? (canRetry ? 'pointer' : 'wait') : 'default'}
    >
      {networkUp && pendingQueries && (
        <>
          <Text>{t('network_status_updating', 'Updating')}</Text>
          <Icon spin ml={2} name="circle-notch" size={20} />
        </>
      )}
      {!networkUp && (
        <>
          <Text fontSize={4} mr={2}>
            {t('connection_lost_notification', {
              nextPingTime: `PT${nextPingTime}S`,
            })}
          </Text>
          <Button border={1} bg="background" disabled={!canRetry} fontSize={4}>
            {t('check_network_connection_btn', {
              nextPingTime: `PT${nextPingTime}S`,
            })}
          </Button>
        </>
      )}
    </Flex>
  );

export default compose(
  withTranslation('common'),
  inject(({ network }) => ({
    networkUp: network.networkUp,
    pendingQueries: !!network.pendingQueries.length,
    nextPingTime: network.nextPingTime,
    checkAgain: network.forceCheck,
    canRetry: network.canRetry,
  })),
  observer
)(NetworkStatus);
