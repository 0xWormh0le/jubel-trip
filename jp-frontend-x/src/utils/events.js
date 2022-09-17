export const stopEvent = (event) => {
  event.preventDefault();
  event.stopPropagation();
};

// export const processKey = ({ key, close, closeOnEscape, closeOnEnter }) => {
//   console.log({ key, close, closeOnEscape, closeOnEnter });
//   switch (key) {
//     case 'Enter':
//       if (closeOnEnter) close();
//       break;
//     case 'Escape':
//       if (closeOnEscape) close();
//       break;
//     default:
//       break;
//   }
// };
