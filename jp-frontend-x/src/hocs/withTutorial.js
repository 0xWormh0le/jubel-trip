import React, { useCallback, useEffect, useState } from 'react';
import introJs from 'intro.js';

const TUTORIALS_NAME = 'jubel-tutorials';

const getTutorialsState = () =>
  JSON.parse(localStorage.getItem(TUTORIALS_NAME)) || {};
const saveTutorialsState = (state) =>
  localStorage.setItem(TUTORIALS_NAME, JSON.stringify(state));

/*
 * @data type
 *   selector: string
 *   intro: string
 *   position?: string
 */
const withTutorial = (tutorialName, data) => (Component) => {
  const saveTutorialsStateHandler = () => {
    const tutorialsState = getTutorialsState();
    tutorialsState[tutorialName] = true;
    saveTutorialsState(tutorialsState);
  };

  return (props) => {
    const [instance, setInstance] = useState(null);
    const startTutorial = useCallback(() => {
      const tutorialsState = getTutorialsState();
      if (tutorialsState[tutorialName] || instance) {
        return;
      }

      data.forEach((item, index) => {
        const elem = document.querySelector(item.selector);
        if (elem) {
          elem.setAttribute('data-intro', item.intro);
          elem.setAttribute('data-step', index + 1);
          if (item.position) {
            elem.setAttribute('data-position', item.position);
          }
        }
      });

      const tutorialInstance = introJs()
        .setOptions({
          scrollTo: 'off',
          showStepNumbers: false,
        })
        .start()
        .onexit(() => {
          data.forEach((item) => {
            const elem = document.querySelector(item.selector);
            if (elem) {
              elem.removeAttribute('data-intro');
              elem.removeAttribute('data-step');
              if (item.position) {
                elem.removeAttribute('data-position');
              }
            }
          });
        })
        .onskip(saveTutorialsStateHandler)
        .oncomplete(saveTutorialsStateHandler);
      setInstance(tutorialInstance);
    }, [instance]);

    useEffect(() => {
      const exitTutorialHandler = () => instance.exit();
      if (instance) {
        data.forEach((item) => {
          const elem = document.querySelector(item.selector);
          if (elem) {
            elem.addEventListener('click', exitTutorialHandler);
          }
        });
      }
      return () => {
        if (instance) {
          instance.exit();
          data.forEach((item) => {
            const elem = document.querySelector(item.selector);
            if (elem) {
              elem.removeEventListener('click', exitTutorialHandler);
            }
          });
        }
      };
    }, [instance]);

    return <Component {...props} startTutorial={startTutorial} />;
  };
};

export default withTutorial;
