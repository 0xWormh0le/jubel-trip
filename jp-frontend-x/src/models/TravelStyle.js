export class TravelStyle {
  name;
  abbr;
  description;
  video;
  image;
  colorPrefix;
  property;

  constructor(props) {
    [
      'name',
      'abbr',
      'colors',
      'description',
      'video',
      'image',
      'property',
    ].forEach((p) => {
      if (props.hasOwnProperty(p)) this[p] = props[p];
    });
    this.colorPrefix = `misc.branding.styles.${this.abbr}`;
  }

  get color() {
    return `${this.colorPrefix}.bg`;
  }

  get disabledColor() {
    return `${this.colorPrefix}.disabled.bg`;
  }

  get onColor() {
    return `${this.colorPrefix}.text`;
  }

  get textColor() {
    return this.onColor;
  }

  get borderColor() {
    return `${this.colorPrefix}.border`;
  }

  get disabledOnColor() {
    return `${this.colorPrefix}.disabled.text`;
  }

  get disabledTextColor() {
    return this.disabledOnColor;
  }

  get disabledBorderColor() {
    return `${this.colorPrefix}.disabled.border`;
  }
}

export const oceanist = new TravelStyle({
  name: 'Oceanist',
  abbr: 'oc',
  property: 'oceanist',
  description:
    "Turquoise waters and powdery sands are your happy place. The ocean's waves bring you in, while its tides sway you into a trance. Whether it be an idyllic island, rocky coastline, or virgin beach, you love to smell the ocean’s salty breeze, swim in its waters, and daydream on its shores. We all once came from the sea, and it is here that you belong.",
  video: 'https://player.vimeo.com/video/299331480?title=0&byline=0&portrait=0',
  image:
    'https://static1.squarespace.com/static/594336f837c5819eb6378ac7/t/5b875c4c88251bea0a25a10f/1535597651268/jakob-owens-300986-unsplash.jpg?format=2500w',
});

export const activeNature = new TravelStyle({
  name: 'Active Nature',
  abbr: 'an',
  property: 'active nature',
  description:
    'The thrill of the moment inspires you. You thrive on the edge of a cliff, on a surf break, or exploring the darkness at the end of the tunnel. The only thing you fear is not feeling alive. You love destinations with plenty of adventurous activities such as remote trekking, mountain biking, motorsports, skydiving, watersports, and white water rafting, to name a few.',
  video: 'https://player.vimeo.com/video/299329259?title=0&byline=0&portrait=0',
  image:
    'https://images.squarespace-cdn.com/content/v1/594336f837c5819eb6378ac7/1561649628576-OSAOG0UDZHTYLOVR60WC/ke17ZwdGBToddI8pDm48kBD1GRV_NMeYtlg7Pvr_8FR7gQa3H78H3Y0txjaiv_0fDoOvxcdMmMKkDsyUqMSsMWxHk725yiiHCCLfrh8O1z5QPOohDIaIeljMHgDF5CVlOqpeNLcJ80NK65_fV7S1UbrpzTMbv1UZMLdNXhO_lJn-ogVEj6whPmt4fIWvXlPkFW-19vHGdlvPV5aDhCUMaA/jakob-owens-NIwLcsJb8Q4-unsplash.jpg',
});

export const chillNature = new TravelStyle({
  name: 'Chill Nature',
  abbr: 'cn',
  property: 'chill nature',
  description:
    'Nature is the opium of your tribe. Feeling connected with the purest places in the world is your peaceful euphoria.  Running water is music to your ears, shooting stars are fireworks to your eyes, and the bare earth beneath your feet is your sacred home.  To you, remote destinations with pristine nature, awe-inspiring landscapes, and wonderful wildlife are heaven on earth.',
  video: 'https://player.vimeo.com/video/299330533?title=0&byline=0&portrait=0',
  image:
    'https://images.squarespace-cdn.com/content/v1/594336f837c5819eb6378ac7/1558567584650-XOLPXJRPQTWUNUI6S3CP/ke17ZwdGBToddI8pDm48kOUni9Rgp-7WnpUTZs3ds-97gQa3H78H3Y0txjaiv_0fDoOvxcdMmMKkDsyUqMSsMWxHk725yiiHCCLfrh8O1z5QHyNOqBUUEtDDsRWrJLTmbFTSCh3GLTYkz25V6Y8urHe1_mkYdJUEYX79XQ_kRg_sXfkdCNjuQ403gflnTuGM/casey-horner-487085-unsplash.jpg?format=2500w',
});

export const culturist = new TravelStyle({
  name: 'Culturist',
  abbr: 'cu',
  property: 'culture',
  description:
    'You are a modern day time traveler looking for your next stop in history. Experiencing traditions that have stood the test of time with magnificent beauty and grace gives you goosebumps. You understand that cultures, both new and old, breathe such authentic life and color into the world that you cannot dare to miss it. You seek destinations with a remarkable past, ancient traditions, unique festivals, striking art, and delicious gastronomy.',
  video: 'https://player.vimeo.com/video/299331190?title=0&byline=0&portrait=0',
  image:
    'https://images.squarespace-cdn.com/content/v1/594336f837c5819eb6378ac7/1558567753908-C16VVWHWAVJ39AYKRPKO/ke17ZwdGBToddI8pDm48kFcjIIX1FKcdO91v_2sr_2x7gQa3H78H3Y0txjaiv_0fDoOvxcdMmMKkDsyUqMSsMWxHk725yiiHCCLfrh8O1z5QPOohDIaIeljMHgDF5CVlOqpeNLcJ80NK65_fV7S1USW_WVzW7aVXo2Ry4s1rbuMMhsaMdDaYTr6tab37c5BbKULy2O2411SvyKR8oCtcyw/beautiful+arches+in+mosaic+building.jpg?format=2500w',
});

export const partyPurist = new TravelStyle({
  name: 'Party Purist',
  abbr: 'pp',
  property: 'partying',
  description:
    'You are a child of the night, free-spirited, and young at heart. Building memories in amazing places with the people you love is all you need. You cannot get enough of meeting new faces and discovering the local scene. You crave destinations were you will never be bored, filled to the brim with cozy bars, cool live music venues, dazzling nightclubs, buzzing restaurants, and energizing festivals.',
  video: 'https://www.youtube.com/embed/0LuiUtv2kug',
  image:
    'https://static1.squarespace.com/static/594336f837c5819eb6378ac7/t/5b8763d603ce64608234e062/1535599579757/alex-knight-365781-unsplash.jpg?format=2500w',
});

export const travelStyles = [
  oceanist,
  activeNature,
  chillNature,
  culturist,
  partyPurist,
];

export const travelStylesByAbbr = travelStyles.reduce((t, ts) => {
  t[ts.abbr] = ts;
  return t;
}, {});

export default TravelStyle;
