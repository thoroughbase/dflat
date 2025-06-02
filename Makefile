BUILD_DIR := build
DFLAT_DYLIB_TARGET := $(BUILD_DIR)/libdflat.dylib # OSX convention
INCLUDE_DIR := include
INCLUDE := $(addprefix -I,$(INCLUDE_DIR))
CXXFLAGS := -Wall -Werror -pedantic -std=c++20
CPPFLAGS := $(INCLUDE) -MMD -MP
LDRPATH := /usr/local/lib
LIBRARIES := -lbuxtehude
LDFLAGS := $(LIBRARIES) -shared -dynamiclib -rpath $(LDRPATH) -install_name @rpath/libdflat.dylib
DFLAT_SOURCE := $(wildcard src/*.cpp)
DFLAT_OBJECTS := $(DFLAT_SOURCE:%.cpp=$(BUILD_DIR)/%.o)
DFLAT_DEPENDENCIES := $(DFLAT_OBJECTS:%.o=%.d)

$(DFLAT_DYLIB_TARGET): $(DFLAT_OBJECTS)
	$(CXX) $(LDFLAGS) $^ -o $@

$(BUILD_DIR)/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

.PHONY: clean
clean:
	rm -r $(BUILD_DIR)

-include $(DFLAT_DEPENDENCIES)
