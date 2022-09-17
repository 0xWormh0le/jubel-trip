import Add from 'emotion-icons/material/Add';
import ArrowBack from 'emotion-icons/material/ArrowBack';
import ArrowDropDown from 'emotion-icons/material/ArrowDropDown';
import ArrowForward from 'emotion-icons/material/ArrowForward';
import Book from 'emotion-icons/fa-solid/Book';
import Cake from 'emotion-icons/material/Cake';
import Calendar from 'emotion-icons/ion-md/Calendar';
import ChatBubbleOutline from 'emotion-icons/material/ChatBubbleOutline';
import Check from 'emotion-icons/fa-solid/Check';
import CheckBox from 'emotion-icons/material/CheckBox';
import CheckBoxOutlineBlank from 'emotion-icons/material/CheckBoxOutlineBlank';
import Circle from 'emotion-icons/fa-solid/Circle';
import CircleNotch from 'emotion-icons/fa-solid/CircleNotch';
import Clear from 'emotion-icons/material/Clear';
import Comment from 'emotion-icons/octicons/Comment';
import Compress from 'emotion-icons/fa-solid/Compress';
import DirectionsBike from 'emotion-icons/material/DirectionsBike';
import DirectionsBoat from 'emotion-icons/material/DirectionsBoat';
import DirectionsBus from 'emotion-icons/material/DirectionsBus';
import DirectionsCar from 'emotion-icons/material/DirectionsCar';
import DirectionsRailway from 'emotion-icons/material/DirectionsRailway';
import DirectionsRun from 'emotion-icons/material/DirectionsRun';
import DirectionsSubway from 'emotion-icons/material/DirectionsSubway';
import DirectionsWalk from 'emotion-icons/material/DirectionsWalk';
import DotsVertical from 'emotion-icons/boxicons-regular/DotsVerticalRounded';
import DriveEta from 'emotion-icons/material/DriveEta';
import Expand from 'emotion-icons/fa-solid/Expand';
import FilterHdr from 'emotion-icons/material/FilterHdr';
import Flag from 'emotion-icons/material/Flag';
import Flight from 'emotion-icons/material/Flight';
import Forum from 'emotion-icons/material/Forum';
import GripLinesVertical from 'emotion-icons/fa-solid/GripLinesVertical';
import Hotel from 'emotion-icons/material/Hotel';
import Info from 'emotion-icons/material/Info';
import KeyboardArrowDown from 'emotion-icons/material/KeyboardArrowDown';
import KeyboardArrowLeft from 'emotion-icons/material/KeyboardArrowLeft';
import KeyboardArrowRight from 'emotion-icons/material/KeyboardArrowRight';
import KeyboardArrowUp from 'emotion-icons/material/KeyboardArrowUp';
import LocalTaxi from 'emotion-icons/material/LocalTaxi';
import MapMarkerAlt from 'emotion-icons/fa-solid/MapMarkerAlt';
import MapRegular from 'emotion-icons/fa-regular/Map';
import MapSolid from 'emotion-icons/fa-solid/Map';
import MapMarked from 'emotion-icons/fa-solid/MapMarked';
import Menu from 'emotion-icons/material/Menu';
import Minus from 'emotion-icons/fa-solid/Minus';
import MonochromePhotos from 'emotion-icons/material/MonochromePhotos';
import Notifications from 'emotion-icons/material/Notifications';
import PersonAdd from 'emotion-icons/material/PersonAdd';
import PlayArrow from 'emotion-icons/material/PlayArrow';
import Plus from 'emotion-icons/fa-solid/Plus';
import PrimitiveDot from 'emotion-icons/octicons/PrimitiveDot';
import QuestionCircle from 'emotion-icons/fa-regular/QuestionCircle';
import RadioButtonChecked from 'emotion-icons/material/RadioButtonChecked';
import RadioButtonUnchecked from 'emotion-icons/material/RadioButtonUnchecked';
import Search from 'emotion-icons/material/Search';
import Share from 'emotion-icons/material/Share';
import Star from 'emotion-icons/material/Star';
import StarBorder from 'emotion-icons/material/StarBorder';
import Sunny from 'emotion-icons/material/WbSunny';
import ToggleOff from 'emotion-icons/fa-solid/ToggleOff';
import ToggleOn from 'emotion-icons/fa-solid/ToggleOn';
import Tune from 'emotion-icons/material/Tune';
import UnfoldLess from 'emotion-icons/material/UnfoldLess';
import UnfoldMore from 'emotion-icons/material/UnfoldMore';
import User from 'emotion-icons/fa-solid/User';
import Work from 'emotion-icons/material/Work';
import WindowMaximum from 'emotion-icons/fa-solid/WindowRestore';
import WindowRestore from 'emotion-icons/fa-regular/WindowRestore';

// the Icon design system component uses these names to create icons
// we import the config any icons we need here
const icons = {
  'arrow-back': ArrowBack,
  'arrow-down': ArrowDropDown,
  'arrow-drop-down': KeyboardArrowDown,
  'arrow-drop-up': KeyboardArrowUp,
  'arrow-forward': ArrowForward,
  'arrow-left': KeyboardArrowLeft,
  'arrow-play': PlayArrow,
  'arrow-right': KeyboardArrowRight,
  'check-box': CheckBox,
  'circle-notch': CircleNotch,
  'directions-run': DirectionsRun,
  'directions-walk': DirectionsWalk,
  'dots-vertical': DotsVertical,
  'drag-vertical': GripLinesVertical,
  'empty-circle': RadioButtonUnchecked,
  'full-circle': RadioButtonChecked,
  'live-chat': Forum,
  'map-marked': MapMarked,
  'map-marker': MapMarkerAlt,
  'map-regular': MapRegular,
  'map-solid': MapSolid,
  'my-trips': Work,
  'person-add': PersonAdd,
  'photo-camera': MonochromePhotos,
  'question-circle': QuestionCircle,
  'radio-button-checked': RadioButtonChecked,
  'radio-button-unchecked': RadioButtonUnchecked,
  'review-icon': ChatBubbleOutline,
  'star-border': StarBorder,
  'surprise-trips': Cake,
  'toggle-off': ToggleOff,
  'toggle-on': ToggleOn,
  'transport-bike': DirectionsBike,
  'transport-boat': DirectionsBoat,
  'transport-bus': DirectionsBus,
  'transport-car': DirectionsCar,
  'transport-flight': Flight,
  'transport-subway': DirectionsSubway,
  'transport-taxi': LocalTaxi,
  'transport-train': DirectionsRailway,
  'unchecked-box': CheckBoxOutlineBlank,
  'unfold-less': UnfoldLess,
  'unfold-more': UnfoldMore,
  add: Add,
  airplane: Flight,
  calendar: Calendar,
  check: Check,
  circle: Circle,
  clear: Clear,
  close: Clear,
  comment: Comment,
  compress: Compress,
  dot: PrimitiveDot,
  drive: DriveEta,
  expand: Expand,
  flag: Flag,
  hotel: Hotel,
  info: Info,
  menu: Menu,
  minus: Minus,
  notifications: Notifications,
  plus: Plus,
  prefs: Tune,
  scenery: FilterHdr,
  search: Search,
  share: Share,
  star: Star,
  stories: Book,
  sunny: Sunny,
  user: User,
  'window-maximum': WindowMaximum,
  'window-restore': WindowRestore,
};

export default icons;
