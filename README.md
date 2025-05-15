# Cone_Search

Python script for astronomers to retrieve and download European Southern Observatory data.

Cone Search Documentation This python script access the ESO Science Archive through Tabular Access Protocol (TAP). The TAP service is used to execute custom queries via SQL-based Astronomical Data Query Language. The science raw frames and reduced data are retrieved from dbo.raw and ivoa.ObsCore table respectively. The target coordinates such as right ascension and declination are resolved through SIMBAD. Using the coordinates of target, a cone search is performed to retrieve the data from ESO archive. The search is conducted within a user-defined radius (in arcminutes) centered on the target position, specified by its right ascension (RA) and declination (Dec).

Upon authorization, it downloads both proprietary and non-proprietary files, which include raw science files with calibration data as well as processed (Phase 3) data. It displays a summary of the retrieved proprietary and non-proprietary files; however, it downloads both sets of files only if authorization is granted; otherwise, it downloads the non-proprietary and calibration files. It also prints any alerts or warnings encountered during the process that generates the calibration cascade.

Users have the flexibility to apply filters on spectroscopic instruments and observation dates. Both processed data and raw science frames are available for download upon request. Additionally, users can specify a destination folder for downloading the data.
Please refer to the user guide for detailed instructions on how to use the cone_search Python script.


If you encounter any errors or unexpected behavior while using this script, please open an issue in this repository.

When reporting, please include:
- A description of the error
- The exact error message and traceback (if available)
- Your Python version and operating system
