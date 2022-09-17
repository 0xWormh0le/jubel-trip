import React from 'react';

import { jubelLogo } from '../assets';
import Flex from '../design-system/Flex';
import Image from '../design-system/Image';
import Link from '../design-system/Link';
import MaxPanel from '../design-system/MaxPanel';
import Text from '../design-system/Text';

const MobileNotReady = () => (
  <MaxPanel p={4} alignItems="center" justifyContent="center">
    <Flex alignItems="center" mt={2} ml={3} mr={4}>
      <Link to="/">
        <Image src={jubelLogo} height={40} width="auto" />
      </Link>
    </Flex>
    <Text mt={4} fontSize={6} fontWeight="bold" uppercase center>
      Mobile Website Under Construction.
    </Text>
    <Text mt={4} center lineHeight="copy">
      Please Check Back In With A Tablet or Computer.
    </Text>
  </MaxPanel>
);

export default MobileNotReady;
