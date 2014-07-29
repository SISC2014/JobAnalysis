Summary: A program that retrieves data from Condor collectors and sends it to Mongo 
Name: condor-retrieval
Version: 1.0.0
Release: 1
License: MIT
Group: System Environment/Daemons
URL: http://dhtc.io
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

Requires: python-futures

%description
The condor_retrieval.py program polls Condor collectors for data and inputs the data into a Mongo database. 

%prep
%setup -q

%build

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/%{_bindir} $RPM_BUILD_ROOT/%{_sysconfdir}/condor_retrieval $RPM_BUILD_ROOT/%{_initddir}
cp condor_retrieval.py $RPM_BUILD_ROOT/%{_bindir}/condor_retrieval.py
cp config.ini $RPM_BUILD_ROOT/%{_sysconfdir}/condor_retrieval/config.ini
cp condor-retrieval $RPM_BUILD_ROOT/%{_initddir}/condor-retrieval

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc
%{_bindir}/condor_retrieval.py
%{_sysconfdir}/condor_retrieval/config.ini
%{_initddir}/condor-retrieval

%changelog
* Fri Jul 25 2014 Anton Yu <antonyu@login01.osgconnect.net> - 1.0.0-1
- Initial build.

