# FinMonitor

A prototype of on-line monitoring system for a list of financial instruments.

## Motivation

This is a playground for testing automated low frequency strategies based on Pandas and freely available financial data. The background reading for the project was [Mastering Pandas for Finance by Michael Heydt]( https://www.amazon.com/Mastering-Pandas-Finance-Michael-Heydt/dp/1783985100) that got me thinking about how far it is possible to push the limits of automated system built on freely available APIs and financial data.

## Configuration

### Crontab on Linux

To update the database with historical prices once a day

`30 22 * * 1-5 ~/SRC/finmonitor/findbupdate.sh >> ~/SRC/finmonitor/findbupdate.out 2>&1`

To update intraday with specified frequency

`42 9-15 * * 1-5 ~/SRC/finmonitor/finmonitor.sh >> ~/SRC/finmonitor/finmonitor.out 2>&1`

### Intraday batch on Windows (run through the Windows scheduler)
`finmonitor.bat`

## Authors

* **Sergei Belenki** - *Initial work*

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

[Mastering Pandas for Finance by Michael Heydt]( https://www.amazon.com/Mastering-Pandas-Finance-Michael-Heydt/dp/1783985100)
