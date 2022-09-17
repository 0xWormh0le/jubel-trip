import React, { useEffect, useState } from 'react';

const withHubspotDetect = (Component) => {
  return (props) => {
    const [hubspotLoaded, setHubspotLoaded] = useState(
      Boolean(window.HubSpotConversations)
    );

    useEffect(() => {
      window.hsConversationsOnReady = [
        ...(window.hsConversationsOnReady || []),
        () => {
          setHubspotLoaded(true);
        },
      ];
    });

    return <Component {...props} hubspotLoaded={hubspotLoaded} />;
  };
};

export default withHubspotDetect;
