import React, { useEffect, useState } from 'react';
import throttle from 'lodash/throttle';

import MobileNotReady from '../screens/MobileNotReady';

const TABLET_UP_WIDTH = 768;

const isMobileScreen = () => window.innerWidth < TABLET_UP_WIDTH;

const withMobileDetect = (Component) => {
  return (props) => {
    const [isMobile, setIsMobile] = useState(isMobileScreen());

    useEffect(() => {
      const handleResize = throttle(() => {
        let newIsMobile = isMobile;
        if (isMobileScreen()) {
          newIsMobile = true;
        } else {
          newIsMobile = false;
        }
        if (newIsMobile !== isMobile) {
          setIsMobile(newIsMobile);
        }
      }, 500);

      window.addEventListener('resize', handleResize);

      return () => {
        window.removeEventListener('resize', handleResize);
      };
    });

    return isMobile ? <MobileNotReady /> : <Component {...props} />;
  };
};

export default withMobileDetect;
