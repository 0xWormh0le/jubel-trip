import React, { useEffect, useState } from 'react';

const McEmbeddableWidget = ({ widgetId }) => {
  const [mcLoaded, setMcLoaded] = useState(Boolean(window.MC));

  useEffect(() => {
    const checkMcLoaded = () => {
      if (window.MC) {
        setMcLoaded(true);
        clearInterval(timerId);
      }
    };
    const timerId = setInterval(() => {
      checkMcLoaded();
    }, 300);
  });

  useEffect(() => {
    if (mcLoaded) {
      window.MC.parse();
    }
  }, [mcLoaded]);

  return (
    <div className="mcwidget-embed" data-widget-id={widgetId}>
      Loading ...
    </div>
  );
};

export default McEmbeddableWidget;
