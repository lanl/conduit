Name:           conduit-slurm-plugin
Version:        0.1.0
Release:        2%{?dist}
Summary:        Conduit Slurm burst buffer Lua plugin with conduit-cli
License:        MIT
URL:            https://github.com/lanl/conduit
Source0:        %{name}-%{version}.tar.gz

BuildArch:      %{_arch}

# Runtime dependencies
Requires:       slurm >= 24.05
Requires:       (lua >= 5.1 or lua53)
Requires:       (lua-posix or luaposix or lua53-luaposix)

%global debug_package %{nil}

%description
Installs a prebuilt conduit-cli used by a Slurm burst buffer Lua plugin,
and the Lua plugin script itself at /etc/conduit/burst_buffer.lua. 

%prep
%setup -q

%build
# No build needed (prebuilt binary)
:

%install
# Install backend binary into /usr/bin
install -Dpm 0755 conduit %{buildroot}%{_sbindir}/conduit

# Install Lua plugin as configuration file for slurm
install -Dpm 0644 burst_buffer.lua %{buildroot}%{_sysconfdir}/conduit/burst_buffer.lua

# Install Lua plugin as configuration file for slurm
install -Dpm 0644 burst_buffer.conf %{buildroot}%{_sysconfdir}/conduit/burst_buffer.conf

%files
%doc slurm-plugin.md
# %license LICENSE
%{_sbindir}/conduit
%config %{_sysconfdir}/conduit/burst_buffer.lua
%config %{_sysconfdir}/conduit/burst_buffer.conf

%changelog
* Mon May 04 2026 Kevin Pelzel <kpelzel@lanl.gov> - 0.1.0-2
- Install to /etc/conduit instead of /etc/slurm
* Tue Dec 02 2025 Kevin Pelzel <kpelzel@lanl.gov> - 0.1.0-1
- Initial package
